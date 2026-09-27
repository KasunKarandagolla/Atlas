from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from types import SimpleNamespace

import pytest
from v2.test_session014_core import KEY

from atlas.domain.execution import Approval
from atlas.domain.risk import engineering_default_policy
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.runtime.assisted_control import AssistedControlShell
from atlas.runtime.phase4_v2 import (
    BINANCE_REQUIRED_CAPABILITIES_V2,
    BYBIT_REQUIRED_CAPABILITIES_V2,
    QualificationStatusV2,
    VenueCapabilityProfileV2,
    VenueCapabilityRowV2,
    _attest_v2_bridge_authority,
    _require_current_ops_refs,
    persist_v2_bridged_trade_plan,
    prepare_v2_bridge_entry,
    validate_venue_profile_for_capital,
)
from atlas.runtime.v2_capital_authority import (
    V2CapitalAuthorityAttestation,
    V2CapitalAuthorityStatus,
    V2LiveAuthorityEvidence,
    V2LiveAuthorityEvidenceKind,
)
from atlas.runtime.writer_lock import WriterLock
from atlas.v2._serialization import sha256_json
from atlas.v2.contracts import ArtifactEnvelope, TradePlanEnvelopeV2, V2Side
from atlas.v2.instruments import EnvironmentV2, VenueV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository

_ACCOUNT = sha256_json("synthetic-redacted-account")
_PRODUCT = sha256_json("product-revision")
_CAPABILITY_SNAPSHOT = sha256_json("capability-snapshot")
_LOCK = sha256_json("dependency-lock")
_ARTIFACT = sha256_json("nautilus-artifact")
_PROTECTION = sha256_json("protection-profile")
_FEE = sha256_json("fee-revision")
_FILTER = sha256_json("filter-revision")
_CUTOFF = 1_800_000_000_000_000_100


def _ref(value: object) -> str:
    return sha256_json(value)


def _ops_qualified_profile(repo: OpsRepository) -> VenueCapabilityProfileV2:
    rows = []
    for capability in BYBIT_REQUIRED_CAPABILITIES_V2:
        metadata = {
            "qualification_class": "AUTHENTICATED_TESTNET",
            "venue": VenueV2.BYBIT.value,
            "environment": EnvironmentV2.TESTNET.value,
            "capability": capability,
            "account_identity_hash": _ACCOUNT,
            "product_ref": _PRODUCT,
            "instrument_key_ref": KEY.content_hash,
            "position_mode": "ONE_WAY",
            "margin_mode": "ISOLATED",
            "nautilus_artifact_ref": _ARTIFACT,
            "dependency_lock_hash": _LOCK,
            "protection_profile_ref": _PROTECTION,
            "fee_revision_ref": _FEE,
            "filter_revision_ref": _FILTER,
            "sensitive_fields_excluded": True,
        }
        evidence_ref = sha256_json(metadata)
        repo.register_artifact(
            ArtifactIndexEntryV2(
                evidence_ref,
                "AuthenticatedVenueQualificationEvidenceV2",
                evidence_ref,
                _CUTOFF - 10,
                _CUTOFF - 10,
                metadata,
            )
        )
        rows.append(
            VenueCapabilityRowV2(
                capability,
                QualificationStatusV2.TESTED,
                QualificationStatusV2.TESTED,
                (evidence_ref,),
            )
        )
    profile = VenueCapabilityProfileV2(
        VenueV2.BYBIT,
        EnvironmentV2.TESTNET,
        _ACCOUNT,
        _PRODUCT,
        KEY.content_hash,
        "ONE_WAY",
        "ISOLATED",
        "nautilus_trader",
        "2.0.0rc5",
        "1b0a49d2792a9432a3aca3fcb617ce7a630d905e",
        _ARTIFACT,
        _LOCK,
        _PROTECTION,
        _FEE,
        _FILTER,
        _CAPABILITY_SNAPSHOT,
        tuple(rows),
        _CUTOFF - 20,
        _CUTOFF + 10_000_000_000,
        False,
    )
    return profile


def _risk_ops_forgery(repo: OpsRepository) -> str:
    metadata = {
        "account_snapshot_ref": _ref("account snapshot"),
        "account_scope": _ACCOUNT,
        "source_class": "AUTHENTICATED_VENUE_RECONCILIATION",
        "observed_at_ns": _CUTOFF,
        "sensitive_fields_excluded": True,
    }
    evidence_ref = sha256_json(metadata)
    repo.register_artifact(
        ArtifactIndexEntryV2(
            evidence_ref,
            "AuthenticatedVenueRiskObservationV2",
            evidence_ref,
            _CUTOFF,
            _CUTOFF,
            metadata,
        )
    )
    return evidence_ref


def _plan_and_bridge(profile: VenueCapabilityProfileV2):
    v1 = engineering_default_policy(policy_version="authority-test", policy_effective_at_ns=0)
    v2_hash = _ref("risk-v2")
    plan = TradePlanEnvelopeV2(
        ArtifactEnvelope(1, "authority-plan", 100, 100, "authority-test", ()),
        _ref("plan-id"),
        "2.0",
        "2.0",
        KEY,
        _PRODUCT,
        _ACCOUNT,
        _ref("policy"),
        _ref("action"),
        _ref("evaluation"),
        v1.policy_hash(),
        profile.capability_snapshot_ref,
        1,
        V2Side.LONG,
        Decimal("0.01"),
        "ioc-entry",
        Decimal("100"),
        Decimal("90"),
        "MARK_PRICE",
        "fixed-stop",
        300,
        Decimal("10"),
        Decimal("20"),
        Decimal("100"),
        Decimal("2"),
        Decimal("99"),
        200,
    )
    from atlas.runtime.phase4_v2 import V2CapitalBridgeEnvelope

    bridge = V2CapitalBridgeEnvelope(
        _ref("candidate-set"),
        "selected-candidate",
        _ref("candidate"),
        _ref("action-artifact"),
        plan.action_hash,
        _ref("sizing"),
        _ref("sizing"),
        v1.policy_hash(),
        v2_hash,
        plan.evaluation_ref,
        profile.content_hash,
        profile.capability_snapshot_ref,
        _PRODUCT,
        KEY.contract_revision,
        _ref("cost-model"),
        _ACCOUNT,
        VenueV2.BYBIT,
        EnvironmentV2.TESTNET,
        KEY.content_hash,
        "LONG",
        plan.qty_limit,
        plan.collar,
        plan.stop,
        plan.stop_trigger_basis,
        plan.horizon_end_ns,
        plan.expires_at_ns,
        plan.normal_risk,
        plan.stress_risk,
        plan.margin,
        plan.leverage_bound,
        plan.plan_id,
        plan.content_hash,
        "S1:1",
    )
    return plan, bridge, v1, v2_hash


def _live_journal(tmp_path, *, runtime_id: str = "runtime-synthetic"):
    lock = WriterLock(tmp_path / "writer.lock")
    ownership = lock.acquire()
    journal = SQLiteJournal(tmp_path / "live.sqlite")
    journal._bind_v2_live_writer(lock, runtime_id)
    return lock, journal, ownership, runtime_id


def _synthetic_attestation(
    journal: SQLiteJournal,
    ownership,
    runtime_id: str,
    *,
    ops_fingerprint=None,
    ops_ref_override: str | None = None,
):
    account_observation = _ref("account-observation")
    account_snapshot = _ref("account-snapshot")
    reconciliation = _ref("exposure-reconciliation")
    protection = _ref("protection-observation")
    capability = _ref("capability-observation")
    profile = _ref("profile")
    recovery_run = _ref("recovery-run")
    other = [
        (capability, V2LiveAuthorityEvidenceKind.CAPABILITY, "one_way_position_mode", None),
        (account_observation, V2LiveAuthorityEvidenceKind.ACCOUNT_RISK, None, account_snapshot),
        (reconciliation, V2LiveAuthorityEvidenceKind.EXPOSURE_RECONCILIATION, None, None),
        (protection, V2LiveAuthorityEvidenceKind.PROTECTION, None, None),
    ]
    live_refs = []
    for evidence_ref, kind, capability_name, snapshot_hash in other:
        evidence = V2LiveAuthorityEvidence(
            evidence_ref,
            kind,
            _ACCOUNT,
            VenueV2.BYBIT,
            EnvironmentV2.TESTNET,
            KEY.contract_revision,
            profile,
            capability_name,
            snapshot_hash,
            _CUTOFF,
            ownership.writer_id,
            ownership.writer_epoch,
            runtime_id,
            recovery_run,
            "SYNTHETIC_TEST_FIXTURE",
            True,
        )
        journal.append_v2_live_authority_evidence(evidence)
        live_refs.append(evidence.content_hash)
    ops_ref = ops_ref_override or _ref("ops-artifact")
    fingerprint = ops_fingerprint or _ref("ops-fingerprint")
    attestation = V2CapitalAuthorityAttestation(
        _ref("bridge"),
        _ref("trade-plan"),
        profile,
        _ref("capability-snapshot"),
        (capability,),
        _ACCOUNT,
        VenueV2.BYBIT,
        EnvironmentV2.TESTNET,
        KEY.content_hash,
        _PRODUCT,
        KEY.contract_revision,
        _ref("risk-policy-v1"),
        _ref("risk-policy-v2"),
        account_snapshot,
        account_observation,
        _ref("risk-evidence"),
        reconciliation,
        protection,
        (ops_ref,),
        ((ops_ref, fingerprint),),
        tuple(sorted(live_refs)),
        ownership.writer_id,
        ownership.writer_epoch,
        runtime_id,
        recovery_run,
        _CUTOFF,
        _CUTOFF,
        _CUTOFF + 100,
        V2CapitalAuthorityStatus.SYNTHETIC_TEST_ONLY,
        "SYNTHETIC_TEST_FIXTURE",
    )
    journal.append_v2_capital_authority_attestation(attestation)
    return attestation, ops_ref


def test_ops_authenticated_qualification_and_risk_labels_alone_do_not_authorize(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        profile = _ops_qualified_profile(repo)
        validate_venue_profile_for_capital(repo, profile, now_ns=_CUTOFF)
        risk_ref = _risk_ops_forgery(repo)
        risk_entry = repo.get_artifact(risk_ref)
        assert risk_entry is not None
        assert risk_entry.metadata["source_class"] == "AUTHENTICATED_VENUE_RECONCILIATION"

        journal = SQLiteJournal(tmp_path / "live.sqlite")
        shell = AssistedControlShell(
            journal=journal,
            runtime_instance_id="unbound",
            writer_id="unbound",
            writer_epoch=1,
        )
        result = prepare_v2_bridge_entry(
            shell,
            repo=repo,
            bridge=None,
            plan=None,
            capability_profile=profile,
            risk_evidence=None,
            risk_policy_v1=None,
            risk_policy_v2=None,
            account_scope=_ACCOUNT,
            cutoff_ns=_CUTOFF,
            approval_id="unused",
            user_identity="operator",
            v1_revalidation_evidence=None,
            authority_attestation_ref=_ref("no-live-attestation"),
            protection_evidence_ref=_ref("protection"),
        )
        assert result.status == "V2_AUTHORITY_BLOCKED"
        assert journal.load_unresolved_intents() == []
        journal.close()


def test_safe_runtime_only_attestation_api_rejects_unbound_ops_caller(tmp_path):
    profile = VenueCapabilityProfileV2.__new__(VenueCapabilityProfileV2)
    shell = AssistedControlShell(
        journal=SQLiteJournal(tmp_path / "live.sqlite"),
        runtime_instance_id="ops-runtime",
        writer_id="ops-writer",
        writer_epoch=1,
    )
    with OpsRepository(tmp_path / "ops.sqlite") as repo, pytest.raises(
        ValueError, match="active SafeRuntime writer"
    ):
        _attest_v2_bridge_authority(
            shell,
            repo=repo,
            bridge=None,
            plan=None,
            capability_profile=profile,
            risk_evidence=None,
            risk_policy_v1=None,
            risk_policy_v2=None,
            account_scope=_ACCOUNT,
            cutoff_ns=_CUTOFF,
            attested_at_ns=_CUTOFF,
            expires_at_ns=_CUTOFF + 1,
            protection_evidence_ref=_ref("protection"),
            v1_revalidation_evidence=None,
        )


def _attestation_for_binding() -> V2CapitalAuthorityAttestation:
    profile = SimpleNamespace(content_hash=_ref("profile"))
    bridge = SimpleNamespace(
        content_hash=_ref("bridge"),
        venue_capability_snapshot_ref=_ref("capability-snapshot"),
        venue=VenueV2.BYBIT,
        environment=EnvironmentV2.TESTNET,
        instrument_key_ref=KEY.content_hash,
        product_ref=_PRODUCT,
        product_revision=KEY.contract_revision,
    )
    plan = SimpleNamespace(content_hash=_ref("trade-plan"))
    refs = tuple(sorted((_ref("capability-evidence"),)))
    ops_ref = _ref("ops-artifact")
    return V2CapitalAuthorityAttestation(
        bridge.content_hash,
        plan.content_hash,
        profile.content_hash,
        bridge.venue_capability_snapshot_ref,
        refs,
        _ACCOUNT,
        bridge.venue,
        bridge.environment,
        bridge.instrument_key_ref,
        bridge.product_ref,
        bridge.product_revision,
        _ref("v1-policy"),
        _ref("v2-policy"),
        _ref("risk-snapshot"),
        _ref("risk-observation"),
        _ref("risk-evidence"),
        _ref("exposure"),
        _ref("protection"),
        (ops_ref,),
        ((ops_ref, _ref("ops-fingerprint")),),
        (_ref("live-evidence"),),
        "writer",
        3,
        "runtime",
        "recovery-run",
        100,
        100,
        110,
        V2CapitalAuthorityStatus.SYNTHETIC_TEST_ONLY,
        "SYNTHETIC_TEST_FIXTURE",
    )


def _binding_inputs(attestation):
    bridge = SimpleNamespace(
        content_hash=attestation.bridge_hash,
        venue_capability_snapshot_ref=attestation.capability_snapshot_hash,
        venue=attestation.venue,
        environment=attestation.environment,
        instrument_key_ref=attestation.instrument_key_hash,
        product_ref=attestation.product_hash,
        product_revision=attestation.product_revision,
    )
    plan = SimpleNamespace(content_hash=attestation.trade_plan_hash)
    profile = SimpleNamespace(content_hash=attestation.capability_profile_hash)
    return {
        "bridge": bridge,
        "plan": plan,
        "capability_profile": profile,
        "account_identity_hash": attestation.account_identity_hash,
        "v1_risk_policy_hash": attestation.v1_risk_policy_hash,
        "risk_policy_v2_hash": attestation.risk_policy_v2_hash,
        "account_risk_snapshot_hash": attestation.account_risk_snapshot_hash,
        "account_risk_observation_hash": attestation.account_risk_observation_hash,
        "risk_evidence_hash": attestation.risk_evidence_hash,
        "exposure_reconciliation_hash": attestation.exposure_reconciliation_hash,
        "protection_evidence_hash": attestation.protection_evidence_hash,
        "writer_id": attestation.writer_id,
        "writer_epoch": attestation.writer_epoch,
        "runtime_instance_id": attestation.runtime_instance_id,
        "recovery_run_id": attestation.recovery_run_id,
        "cutoff_ns": attestation.evidence_cutoff_ns,
        "now_ns": attestation.evidence_cutoff_ns,
    }


@pytest.mark.parametrize(
    ("field", "replacement", "reason"),
    [
        ("bridge", SimpleNamespace(content_hash=_ref("other-bridge")), "bridge_hash"),
        ("plan", SimpleNamespace(content_hash=_ref("other-plan")), "trade_plan_hash"),
        ("account_identity_hash", _ref("other-account"), "account_identity_hash"),
        ("v1_risk_policy_hash", _ref("other-v1-policy"), "v1_risk_policy_hash"),
        ("risk_policy_v2_hash", _ref("other-v2-policy"), "risk_policy_v2_hash"),
        ("writer_id", "other-writer", "writer_id"),
        ("writer_epoch", 4, "writer_epoch"),
        ("runtime_instance_id", "other-runtime", "runtime_instance_id"),
        ("recovery_run_id", "other-recovery", "recovery_run_id"),
        ("cutoff_ns", 101, "evidence_cutoff_ns"),
    ],
)
def test_attestation_exact_binding_rejects_material_changes(field, replacement, reason):
    attestation = _attestation_for_binding()
    args = _binding_inputs(attestation)
    if field in {"bridge", "plan"}:
        args[field] = SimpleNamespace(**(vars(args[field]) | {"content_hash": replacement.content_hash}))
    else:
        args[field] = replacement
    reasons = attestation.binding_reasons(**args)
    assert any(reason in item for item in reasons)


def test_attestation_binds_capability_profile_snapshot_and_staleness():
    attestation = _attestation_for_binding()
    args = _binding_inputs(attestation)
    args["capability_profile"] = SimpleNamespace(content_hash=_ref("different-profile"))
    assert any("capability_profile_hash" in item for item in attestation.binding_reasons(**args))
    args = _binding_inputs(attestation)
    args["bridge"] = SimpleNamespace(
        **(vars(args["bridge"]) | {"venue_capability_snapshot_ref": _ref("different-snapshot")})
    )
    assert any("capability_snapshot_hash" in item for item in attestation.binding_reasons(**args))
    args = _binding_inputs(attestation)
    args["now_ns"] = attestation.expires_at_ns
    assert any("stale or expired" in item for item in attestation.binding_reasons(**args))


def test_synthetic_live_journal_attestation_is_immutable_and_never_live(tmp_path):
    lock, journal, owner, runtime = _live_journal(tmp_path)
    attestation, _ = _synthetic_attestation(journal, owner, runtime)
    loaded = journal.load_v2_capital_authority_attestation(attestation.content_hash)
    assert loaded == attestation
    assert loaded.status is V2CapitalAuthorityStatus.SYNTHETIC_TEST_ONLY
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        journal._conn.execute(
            "UPDATE v2_capital_authority_attestations SET status='LIVE_ATTESTED' WHERE attestation_hash=?",
            (attestation.content_hash,),
        )
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        journal._conn.execute(
            "DELETE FROM v2_capital_authority_attestations WHERE attestation_hash=?",
            (attestation.content_hash,),
        )
    evidence_ref = attestation.live_evidence_refs[0]
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        journal._conn.execute(
            "UPDATE v2_live_authority_evidence SET synthetic_fixture=0 WHERE evidence_hash=?",
            (evidence_ref,),
        )
    journal.close()
    lock.release()


def test_unbound_journal_cannot_append_v2_authority_records(tmp_path):
    journal = SQLiteJournal(tmp_path / "unbound.sqlite")
    evidence = V2LiveAuthorityEvidence(
        _ref("evidence"),
        V2LiveAuthorityEvidenceKind.CAPABILITY,
        _ACCOUNT,
        VenueV2.BYBIT,
        EnvironmentV2.TESTNET,
        KEY.contract_revision,
        _ref("profile"),
        "one_way_position_mode",
        None,
        _CUTOFF,
        "writer",
        1,
        "runtime",
        "recovery-run",
        "SYNTHETIC_TEST_FIXTURE",
        True,
    )
    with pytest.raises(PersistenceError, match="active writer context"):
        journal.append_v2_live_authority_evidence(evidence)
    journal.close()


def test_ops_artifact_mutation_changes_fingerprint_without_rewriting_attestation(tmp_path):
    repo = OpsRepository(tmp_path / "ops.sqlite")
    ref = _ref("mutable-underlying-row")
    repo.register_artifact(ArtifactIndexEntryV2(ref, "test", ref, 1, 1, {"value": "before"}))
    old_fingerprint = _require_current_ops_refs(repo, (ref,))
    lock, journal, owner, runtime = _live_journal(tmp_path)
    attestation, ops_ref = _synthetic_attestation(
        journal,
        owner,
        runtime,
        ops_fingerprint=old_fingerprint[0][1],
        ops_ref_override=ref,
    )
    # The record is content-addressed and immutable even if ops.sqlite is edited out of band.
    before = journal.load_v2_capital_authority_attestation(attestation.content_hash)
    repo._connection.execute(
        "UPDATE artifact_index SET metadata_json=? WHERE artifact_ref=?",
        (json.dumps({"value": "after"}, separators=(",", ":")), ref),
    )
    changed_fingerprint = _require_current_ops_refs(repo, (ref,))
    assert changed_fingerprint != old_fingerprint
    assert changed_fingerprint != before.ops_artifact_fingerprints
    assert journal.load_v2_capital_authority_attestation(attestation.content_hash) == before
    assert ops_ref in attestation.ops_artifact_refs
    repo.close()
    journal.close()
    lock.release()


def test_recovery_required_state_invalidates_authority_before_attestation(tmp_path):
    lock, journal, owner, runtime = _live_journal(tmp_path)
    with pytest.raises(ValueError, match="current live recovery certificate is missing"):
        from atlas.runtime.phase4_v2 import _require_v2_current_recovery

        _require_v2_current_recovery(
            journal,
            recovery_run_id=_ref("missing-recovery"),
            writer_id=owner.writer_id,
            writer_epoch=owner.writer_epoch,
            runtime_instance_id=runtime,
        )
    current_run = _ref("recovery-required")
    cert = SimpleNamespace(
        recovery_run_id=current_run,
        decision=SimpleNamespace(value="RECOVERY_REQUIRED"),
        reconciliation_health=SimpleNamespace(value="CURRENT"),
        writer_id=owner.writer_id,
        writer_epoch=owner.writer_epoch,
        runtime_instance_id=runtime,
        journal_schema_version=6,
        unresolved_intents=(),
        unresolved_commands=(),
        unknown_commands=(),
    )
    journal_with_incident = SimpleNamespace(
        load_latest_recovery_certificate=lambda: cert,
        schema_version=lambda: 6,
    )
    with pytest.raises(ValueError, match="recovery-required"):
        from atlas.runtime.phase4_v2 import _require_v2_current_recovery

        _require_v2_current_recovery(
            journal_with_incident,
            recovery_run_id=current_run,
            writer_id=owner.writer_id,
            writer_epoch=owner.writer_epoch,
            runtime_instance_id=runtime,
        )
    journal.close()
    lock.release()


def test_approval_cannot_replace_missing_attestation_or_create_intent(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        profile = _ops_qualified_profile(repo)
        plan, bridge, v1, v2_hash = _plan_and_bridge(profile)
        journal = SQLiteJournal(tmp_path / "live.sqlite")
        v1_plan = persist_v2_bridged_trade_plan(journal, bridge, plan)
        approval = Approval(
            "approval-authority-test",
            "operator",
            v1_plan.plan_id,
            v1_plan.version,
            v1_plan.created_at_ns + 1,
            v1_plan.expires_at_ns - 1,
        )
        journal.create_approval(approval)
        shell = AssistedControlShell(
            journal=journal,
            runtime_instance_id="test-runtime",
            writer_id="test-writer",
            writer_epoch=1,
        )
        result = prepare_v2_bridge_entry(
            shell,
            repo=repo,
            bridge=bridge,
            plan=plan,
            capability_profile=profile,
            risk_evidence=None,
            risk_policy_v1=v1,
            risk_policy_v2=SimpleNamespace(policy_hash=v2_hash),
            account_scope=_ACCOUNT,
            cutoff_ns=_CUTOFF,
            approval_id=approval.approval_id,
            user_identity="operator",
            v1_revalidation_evidence=None,
        )
        assert result.status == "V2_AUTHORITY_BLOCKED"
        assert journal.load_approval(approval.approval_id).consumed_at_ns is None
        assert journal.load_unresolved_intents() == []
        assert journal.load_unresolved_commands() == []
        journal.close()


def test_different_bridge_attestation_is_rejected_before_approval_or_intent(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        profile = _ops_qualified_profile(repo)
        plan, bridge, v1, v2_hash = _plan_and_bridge(profile)
        lock, journal, owner, runtime = _live_journal(tmp_path)
        attestation, _ = _synthetic_attestation(journal, owner, runtime)
        shell = AssistedControlShell(
            journal=journal,
            runtime_instance_id=runtime,
            writer_id=owner.writer_id,
            writer_epoch=owner.writer_epoch,
            live_writer_lock=lock,
        )
        result = prepare_v2_bridge_entry(
            shell,
            repo=repo,
            bridge=bridge,
            plan=plan,
            capability_profile=profile,
            risk_evidence=None,
            risk_policy_v1=v1,
            risk_policy_v2=SimpleNamespace(policy_hash=v2_hash),
            account_scope=_ACCOUNT,
            cutoff_ns=_CUTOFF,
            approval_id="unused",
            user_identity="operator",
            v1_revalidation_evidence=SimpleNamespace(recovery_run_id=_ref("recovery-run")),
            authority_attestation_ref=attestation.content_hash,
            protection_evidence_ref=_ref("protection"),
        )
        assert result.status == "V2_AUTHORITY_BLOCKED"
        assert any("bridge_hash mismatch" in reason for reason in result.reasons)
        assert journal.load_unresolved_intents() == []
        journal.close()
        lock.release()


def test_only_bybit_required_capability_contract_is_used_for_bybit_profile():
    assert BYBIT_REQUIRED_CAPABILITIES_V2
    assert BINANCE_REQUIRED_CAPABILITIES_V2
