"""Scoped declarations never substitute future, foreign or invented evidence."""
from dataclasses import replace

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.economic_sources import (
    ARTIFACT_TYPE,
    MAX_MANIFESTS,
    EconomicSourceManifestV1,
    declared_execution_model,
    index_economic_source_manifest,
    resolve_economic_source_manifest,
    validate_economic_source_manifest,
)
from atlas.v2.science.pretrade import CausalInputV2
from atlas.v2.science.scenario_engine import scenario_seed
from atlas.v2.strategies.s2_breakout import S2_POLICY

from .test_session017_risk import risk_case
from .test_session020_phase2_e2e import _admission_policy


def _input(repo, case, kind, *, at=None, account=None):
    cutoff = case.candidate.decision_at_ns if at is None else at
    body = {"version": "DECLARED_OFFLINE_RESEARCH_SOURCE_V1", "kind": kind,
        "instrument_key_ref": case.product.key.content_hash, "product_ref": case.product.content_hash,
        "account_scope": case.account.account_scope if account is None else account,
        "available_at_ns": cutoff, "vintage_at_ns": cutoff}
    ref = sha256_json(body)
    repo.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, cutoff, cutoff, body))
    return CausalInputV2(ref, kind, cutoff, cutoff)


@pytest.fixture
def setup(tmp_path):
    with OpsRepository(tmp_path / "declarations.sqlite") as repo:
        case = risk_case(repo)
        cutoff = case.candidate.decision_at_ns
        manifest = EconomicSourceManifestV1(case.product.key.content_hash, case.product.content_hash,
            case.account.account_scope, case.candidate.policy_hash, _admission_policy(),
            _input(repo, case, "DeclaredModelV1"), _input(repo, case, "DeclaredCalibrationV1"),
            _input(repo, case, "ExecutionModelV1"), 100, cutoff, cutoff)
        yield repo, case, manifest


def _resolve(repo, case, cutoff=None, **scope):
    return resolve_economic_source_manifest(repo, product=scope.get("product", case.product),
        account_scope=scope.get("account_scope", case.account.account_scope),
        policy_hash=scope.get("policy_hash", case.candidate.policy_hash),
        cutoff_ns=case.candidate.decision_at_ns if cutoff is None else cutoff)


def test_manifest_roundtrip_exact_sources_identity_and_deterministic_seed(setup):
    repo, case, manifest = setup
    assert EconomicSourceManifestV1.from_dict(manifest.to_dict()) == manifest
    ref = index_economic_source_manifest(repo, manifest)
    assert index_economic_source_manifest(repo, manifest) == ref
    assert _resolve(repo, case) == (manifest, None)
    assert validate_economic_source_manifest(repo, repo.get_artifact(ref), cutoff_ns=manifest.available_at_ns) == manifest
    assert manifest.seed("a" * 64, manifest.available_at_ns) == scenario_seed("a" * 64, manifest.available_at_ns)
    assert declared_execution_model(repo, case.product, case.account.account_scope, manifest.available_at_ns) == manifest.execution_model_input
    assert declared_execution_model(repo, case.product, None, manifest.available_at_ns) is None
    assert manifest.authority == "ZERO"


def test_missing_future_and_foreign_declarations_never_supply_global_fallback(setup):
    repo, case, manifest = setup
    assert _resolve(repo, case)[1] == "ECONOMIC_SOURCE_MANIFEST_MISSING"
    later = replace(manifest, available_at_ns=manifest.available_at_ns + 1, effective_at_ns=manifest.effective_at_ns + 1)
    index_economic_source_manifest(repo, later)
    assert _resolve(repo, case)[0] is None
    assert _resolve(repo, case, cutoff=later.available_at_ns) == (later, None)
    assert _resolve(repo, case, cutoff=later.available_at_ns, account_scope="FOREIGN_ACCOUNT")[0] is None
    assert _resolve(repo, case, cutoff=later.available_at_ns, policy_hash=S2_POLICY.policy_hash)[0] is None
    foreign = replace(case.product, key=replace(case.product.key, native_symbol="FOREIGNUSDT"))
    assert _resolve(repo, case, cutoff=later.available_at_ns, product=foreign)[0] is None
    assert declared_execution_model(repo, foreign, case.account.account_scope, later.available_at_ns) is None


def test_ambiguous_effective_version_abstains_without_older_rescue(setup):
    repo, case, manifest = setup
    index_economic_source_manifest(repo, manifest)
    index_economic_source_manifest(repo, replace(manifest, scenario_count=101))
    assert _resolve(repo, case) == (None, "ECONOMIC_SOURCE_MANIFEST_AMBIGUOUS")
    assert declared_execution_model(repo, case.product, case.account.account_scope, manifest.available_at_ns) is None
    newest = replace(manifest, available_at_ns=manifest.available_at_ns + 1, effective_at_ns=manifest.effective_at_ns + 1)
    index_economic_source_manifest(repo, newest)
    assert _resolve(repo, case, cutoff=newest.available_at_ns) == (newest, None)


def test_execution_inventory_requires_agreement_across_policy_declarations(setup):
    repo, case, manifest = setup
    index_economic_source_manifest(repo, manifest)
    second = replace(manifest, policy_hash=S2_POLICY.policy_hash)
    index_economic_source_manifest(repo, second)
    assert declared_execution_model(repo, case.product, case.account.account_scope, manifest.available_at_ns) == manifest.execution_model_input
    body = {"kind": "ExecutionModelV1", "explicit_revision": 2}
    ref = sha256_json(body)
    repo.register_artifact(ArtifactIndexEntryV2(ref, "ExecutionModelV1", ref,
        manifest.available_at_ns, manifest.available_at_ns, body))
    alternate = CausalInputV2(ref, "ExecutionModelV1", manifest.available_at_ns, manifest.available_at_ns)
    changed = replace(second, execution_model_input=alternate,
        available_at_ns=manifest.available_at_ns + 1, effective_at_ns=manifest.effective_at_ns + 1)
    index_economic_source_manifest(repo, changed)
    assert declared_execution_model(repo, case.product, case.account.account_scope, changed.available_at_ns) is None


@pytest.mark.parametrize("mutation", ["hash", "time", "foreign", "missing", "wrong_type", "body_future"])
def test_source_validation_rejects_canonical_tamper_future_foreign_and_absence(setup, mutation):
    repo, case, manifest = setup
    original = manifest.execution_model_input
    if mutation == "hash":
        body = {"changed": True}
        ref = "b" * 64
        repo.register_artifact(ArtifactIndexEntryV2(ref, "ExecutionModelV1", ref,
            manifest.available_at_ns, manifest.available_at_ns, body))
        replacement = CausalInputV2(ref, "ExecutionModelV1", manifest.available_at_ns, manifest.available_at_ns)
    elif mutation == "time":
        replacement = replace(original, available_at_ns=original.available_at_ns - 1, vintage_at_ns=original.vintage_at_ns - 1)
    elif mutation == "foreign":
        replacement = _input(repo, case, "ExecutionModelV1", account="FOREIGN_ACCOUNT")
    elif mutation == "missing":
        replacement = replace(original, ref="c" * 64)
    elif mutation == "body_future":
        body = {"information_cutoff_ns": manifest.available_at_ns + 1}
        ref = sha256_json(body)
        repo.register_artifact(ArtifactIndexEntryV2(ref, "ExecutionModelV1", ref,
            manifest.available_at_ns, manifest.available_at_ns, body))
        replacement = replace(original, ref=ref)
    else:
        ref = "d" * 64
        repo.register_artifact(ArtifactIndexEntryV2(ref, "WrongTypeV1", ref,
            manifest.available_at_ns, manifest.available_at_ns, {"declared": True}))
        replacement = replace(original, ref=ref)
    with pytest.raises(ValueError):
        index_economic_source_manifest(repo, replace(manifest, execution_model_input=replacement))


def test_unknown_fields_and_canonical_manifest_metadata_are_rejected(setup):
    repo, _, manifest = setup
    with pytest.raises(ValueError):
        EconomicSourceManifestV1.from_dict({**manifest.to_dict(), "hidden_fallback": True})
    indexed = ArtifactIndexEntryV2(manifest.content_hash, ARTIFACT_TYPE, manifest.content_hash,
        manifest.available_at_ns, manifest.available_at_ns,
        {"economic_source_manifest": manifest.to_dict(), "hidden_fallback": True})
    with pytest.raises(ValueError, match="canonical metadata"):
        validate_economic_source_manifest(repo, indexed, cutoff_ns=manifest.available_at_ns)


def test_reference_population_and_optional_unavailable_evidence_are_refused(setup):
    repo, _, manifest = setup
    with pytest.raises(ValueError, match="population"):
        replace(manifest, joint_data_refs=tuple(f"{number:064x}" for number in range(1, 130)))
    with pytest.raises(ValueError, match="missing"):
        index_economic_source_manifest(repo, replace(manifest, support_unit_refs=("e" * 64,)))
    with pytest.raises(ValueError, match="retrospective"):
        replace(manifest, source_inputs=(CausalInputV2("a" * 64, "ActionReplaySourceEvidenceV1", 0, 0),))
    with pytest.raises(ValueError, match="unavailable"):
        replace(manifest, execution_model_input=replace(manifest.execution_model_input,
            available_at_ns=manifest.available_at_ns + 1))


def test_whole_manifest_population_overflow_cannot_silently_select_recent_subset(setup):
    repo, case, manifest = setup
    with repo.atomic_composition():
        for number in range(MAX_MANIFESTS + 1):
            index_economic_source_manifest(repo, replace(manifest, scenario_count=number + 1))
    assert _resolve(repo, case) == (None, "ECONOMIC_SOURCE_MANIFEST_INVALID_OR_OVERFLOW")
    assert declared_execution_model(repo, case.product, case.account.account_scope, manifest.available_at_ns) is None
    pressure = repo.artifact_entries("OpsActiveWorkPressureV1")
    assert len(pressure) == 1
    assert pressure[0].metadata["pressure"]["has_more"] is True
    assert pressure[0].metadata["pressure"]["authority"] == "ZERO"


def test_existing_joint_declaration_is_retained_without_inventing_execution_qualification(tmp_path):
    from .test_session019_scenarios import _fixture

    with OpsRepository(tmp_path / "joint-declaration.sqlite") as repo:
        _, data, scenario, _ = _fixture(repo)
        case = risk_case(repo)
        manifest = EconomicSourceManifestV1(case.product.key.content_hash, case.product.content_hash,
            case.account.account_scope, case.candidate.policy_hash, _admission_policy(),
            scenario.model_input, scenario.calibration_input, scenario.execution_model_input,
            100, scenario.information_cutoff_ns, scenario.information_cutoff_ns,
            source_inputs=scenario.source_inputs, joint_data_refs=(data.content_hash,))
        ref = index_economic_source_manifest(repo, manifest)
        assert _resolve(repo, case) == (manifest, None)
        assert validate_economic_source_manifest(repo, repo.get_artifact(ref),
            cutoff_ns=manifest.available_at_ns).joint_data_refs == (data.content_hash,)
        # Declaration preserves this marker; production cannot promote it to
        # real execution support merely because the manifest is valid.
        assert repo.get_artifact(data.content_hash).metadata["joint_execution_data"]["synthetic_fixture"] is True
